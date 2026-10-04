export const EnterprisePlanFeatureBadge = ({feature='Эта возможность', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Возможность тарифа Enterprise
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'доступны' : 'доступна'} в тарифе Enterprise. {support ? `Чтобы включить эту возможность, обратитесь в службу поддержки.` : 'Чтобы перейти на этот тариф, откройте страницу тарифов в облачной консоли.'}</p>
            </div>
        </div>
    )
}