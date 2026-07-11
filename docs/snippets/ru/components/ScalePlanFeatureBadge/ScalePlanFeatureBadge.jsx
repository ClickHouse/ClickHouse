export const ScalePlanFeatureBadge = ({feature='Эта возможность', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Возможность плана Scale
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'доступны' : 'доступна'} в тарифах Scale и Enterprise. Чтобы перейти на более высокий тариф, откройте страницу тарифов в облачной консоли.</p>
            </div>
        </div>
    )
}