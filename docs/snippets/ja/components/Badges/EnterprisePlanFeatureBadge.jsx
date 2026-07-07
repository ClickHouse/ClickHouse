export const EnterprisePlanFeatureBadge = ({feature='この機能', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Enterprise プランの機能
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'は' : 'は'}Enterprise プランでご利用いただけます。 {support ? `この機能を有効にするには、サポートにお問い合わせください。` : 'アップグレードするには、Cloud Console のプランページをご確認ください。'}</p>
            </div>
        </div>
    )
}