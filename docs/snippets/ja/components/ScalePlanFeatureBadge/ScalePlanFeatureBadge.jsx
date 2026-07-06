export const ScalePlanFeatureBadge = ({feature='この機能', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Scale プランの機能
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'は' : 'は'} Scale および Enterprise プランで利用できます。アップグレードするには、Cloud Console のプランページをご覧ください。</p>
            </div>
        </div>
    )
}